/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_fili.c                                          :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/05 14:43:21 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/07 18:13:41 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	ft_fili(char *file, int line)
{
	if (!file)
		ft_putstr("no file\n");
	else
	{
		ft_putstr(file);
		ft_putstr(", line ");
		ft_putnbr(line);
		ft_putstr("\n");
	}
}
