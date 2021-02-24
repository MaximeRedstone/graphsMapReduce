/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_atoi_base.c                                     :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/07 22:42:55 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/20 14:12:21 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

int		ft_atoi_base(char *nbr, char *base_from)
{
	long int		res;
	long int		sign;
	long int		len;
	long int		pos;

	if (!nbr || !base_from)
		return (0);
	if ((len = nbr ? ft_check_base(nbr, base_from) : 0) < 2)
		return (0);
	res = 0;
	sign = *nbr == 45 ? -1 : 1;
	if (*nbr == 43 || *nbr == 45)
		nbr++;
	while (*nbr && *nbr != 43 && *nbr != 45)
	{
		pos = 0;
		while (base_from[pos] != *nbr)
			pos++;
		res = res * len + pos;
		nbr++;
	}
	return (res * sign);
}
