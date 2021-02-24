/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_atou_mv.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/10/23 17:43:56 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/03 14:44:21 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

long	ft_atou_mv(char **str, char sep)
{
	long	flip;
	long	nb;

	nb = 0;
	while (ft_isspace(**str) && **str != '-' && **str != '+' && **str)
		(*str)++;
	flip = **str == '-' ? -1 : 1;
	if (!(**str == '-' || **str == '+'))
		return (-1);
	while (**str >= '0' && **str <= '9')
		nb = nb * 10 + *(*str++) - 48;
	if (!(**str == sep || **str == '\n'))
		return (-1);
	return (nb * flip);
}
