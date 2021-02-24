/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_check_base.c                                    :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/07 16:57:37 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/10 18:28:43 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

static int		ft_check_base_2(char *base)
{
	int		still;
	int		move;

	if (!base)
		return (0);
	still = -1;
	while (base[++still])
	{
		move = still;
		while (base[++move])
			if ((base[move] == base[still] && move != still) || base[move] == 43
					|| base[move] == 45 || base[move] < 32 || base[move] > 126)
				return (0);
	}
	return (still);
}

int				ft_check_base(char *str, char *base)
{
	int		still;
	int		move;

	if (!str || !base)
		return (0);
	*str == 43 || *str == 45 ? str++ : str;
	still = -1;
	while (str[++still])
	{
		move = -1;
		while (base[++move])
			if (base[move] == str[still])
				break ;
		if (!base[move])
			return (0);
	}
	return (ft_check_base_2(base));
}
